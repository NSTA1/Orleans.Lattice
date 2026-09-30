using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers <see cref="RepoContextGrainStorageWriteGate"/>, which bounds how many
/// grain-storage writes contend for SQLite's single writer at once (issue #2419).
/// </summary>
/// <remarks>
/// The property that matters most here is that the gate <b>fails open</b>. It sits in
/// front of every grain-storage write the host makes, so a bug that made it fail
/// closed would not degrade the host, it would stop it. Two tests below exist only to
/// hold that: a writer that is not admitted proceeds, and a zero timeout never waits.
/// </remarks>
[TestFixture]
public sealed class RepoContextGrainStorageWriteGateTests
{
    private static readonly TimeSpan Generous = TimeSpan.FromSeconds(30);

    [Test]
    public async Task A_gate_with_free_permits_admits_immediately_and_without_queueing()
    {
        using var gate = new RepoContextGrainStorageWriteGate(2, Generous);

        var first = await gate.AcquireAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(RepoContextGrainStorageWriteGateOutcome.Immediate));
            Assert.That(gate.IsBounded, Is.True);
            Assert.That(gate.Admitted, Is.EqualTo(1));
            Assert.That(gate.Queued, Is.Zero);
        });

        gate.Release(first);
        Assert.That(gate.Admitted, Is.Zero);
    }

    [Test]
    public async Task A_writer_beyond_the_bound_queues_and_is_admitted_when_a_permit_is_returned()
    {
        using var gate = new RepoContextGrainStorageWriteGate(1, Generous);
        var held = await gate.AcquireAsync(CancellationToken.None);

        var queued = gate.AcquireAsync(CancellationToken.None).AsTask();
        await WaitUntilAsync(() => gate.Queued == 1);

        Assert.Multiple(() =>
        {
            Assert.That(queued.IsCompleted, Is.False, "The bound must actually hold the second writer back.");
            Assert.That(gate.Admitted, Is.EqualTo(1));
        });

        gate.Release(held);
        var outcome = await queued.WaitAsync(Generous);

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(RepoContextGrainStorageWriteGateOutcome.Queued));
            Assert.That(gate.Queued, Is.Zero);
            Assert.That(gate.Admitted, Is.EqualTo(1));
        });

        gate.Release(outcome);
    }

    [Test]
    public async Task A_writer_that_is_not_admitted_within_the_timeout_proceeds_ungated()
    {
        using var gate = new RepoContextGrainStorageWriteGate(1, TimeSpan.FromMilliseconds(50));
        var held = await gate.AcquireAsync(CancellationToken.None);

        var outcome = await gate.AcquireAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(RepoContextGrainStorageWriteGateOutcome.TimedOut),
                "The gate fails OPEN. It sits in front of every grain-storage write the host makes, "
                + "so its worst case has to be the behaviour that shipped without it - never a "
                + "blocked or failed write.");
            Assert.That(outcome.HoldsPermit(), Is.False, "A writer that was not admitted holds nothing to release.");
            Assert.That(gate.Queued, Is.Zero, "The wait is over, so the writer is no longer queued.");
            Assert.That(gate.Admitted, Is.EqualTo(1), "Only the held permit is still out.");
        });

        gate.Release(outcome);
        Assert.That(gate.Admitted, Is.EqualTo(1),
            "Releasing an outcome that never took a permit must not hand back one it does not own; "
            + "doing so would let the bound drift upward every time a writer timed out.");

        gate.Release(held);
    }

    [Test]
    public async Task A_zero_timeout_never_waits()
    {
        using var gate = new RepoContextGrainStorageWriteGate(1, TimeSpan.Zero);
        var held = await gate.AcquireAsync(CancellationToken.None);

        var pending = gate.AcquireAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pending.IsCompleted, Is.True, "A zero timeout must complete synchronously.");
            Assert.That(pending.Result, Is.EqualTo(RepoContextGrainStorageWriteGateOutcome.TimedOut));
        });

        gate.Release(held);
    }

    [Test]
    public async Task An_unbounded_gate_admits_everything_without_allocating_a_permit()
    {
        var gate = RepoContextGrainStorageWriteGate.Unbounded;

        var outcomes = await Task.WhenAll(Enumerable.Range(0, 32)
            .Select(_ => gate.AcquireAsync(CancellationToken.None).AsTask()));

        Assert.Multiple(() =>
        {
            Assert.That(gate.IsBounded, Is.False);
            Assert.That(outcomes, Is.All.EqualTo(RepoContextGrainStorageWriteGateOutcome.Unbounded));
            Assert.That(gate.Queued, Is.Zero);
            Assert.That(gate.Admitted, Is.Zero);
        });
    }

    [Test]
    public void A_permit_count_of_zero_or_less_bounds_nothing()
    {
        using var zero = new RepoContextGrainStorageWriteGate(0, Generous);
        using var negative = new RepoContextGrainStorageWriteGate(-4, Generous);

        Assert.Multiple(() =>
        {
            Assert.That(zero.IsBounded, Is.False);
            Assert.That(negative.IsBounded, Is.False);
        });
    }

    [Test]
    public async Task The_bound_holds_under_concurrent_writers()
    {
        const int Permits = 4;
        using var gate = new RepoContextGrainStorageWriteGate(Permits, Generous);
        using var release = new SemaphoreSlim(0);
        var concurrent = 0;
        var peak = 0;

        var writers = Enumerable.Range(0, 24).Select(async _ =>
        {
            var admission = await gate.AcquireAsync(CancellationToken.None);
            try
            {
                var now = Interlocked.Increment(ref concurrent);
                InterlockedRaise(ref peak, now);
                await release.WaitAsync(Generous);
                Interlocked.Decrement(ref concurrent);
            }
            finally
            {
                gate.Release(admission);
            }
        }).ToArray();

        await WaitUntilAsync(() => Volatile.Read(ref concurrent) == Permits && gate.Queued == 20);
        release.Release(24);
        await Task.WhenAll(writers).WaitAsync(Generous);

        Assert.Multiple(() =>
        {
            Assert.That(peak, Is.EqualTo(Permits),
                "Twenty-four writers against four permits must never put more than four in flight. "
                + "That cap is the whole remedy: SQLite has one writer, so concurrency beyond a "
                + "small width buys no throughput and converts the surplus into exhausted busy "
                + "windows.");
            Assert.That(gate.Admitted, Is.Zero, "Every permit is handed back.");
            Assert.That(gate.Queued, Is.Zero);
        });
    }

    [Test]
    public void The_defaults_bound_ordinary_work_loosely_and_a_convoy_tightly()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextGrainStorageWriteGate.DefaultPermits, Is.GreaterThan(1),
                "A bound of one would serialise every write in process and is not what the issue asks for.");
            Assert.That(RepoContextGrainStorageWriteGate.DefaultPermits, Is.LessThan(106),
                "106 is the peak concurrency the issue #2419 attribution recorded. A default at or "
                + "above it would admit the whole convoy and bound nothing that matters.");
            Assert.That(RepoContextGrainStorageWriteGate.DefaultAcquireTimeout,
                Is.GreaterThan(TimeSpan.Zero).And.LessThan(TimeSpan.FromSeconds(30)),
                "A writer must give up waiting well inside the 30 s Orleans call timeout, so it "
                + "still has its own busy window ahead of it when it proceeds ungated.");
        });
    }

    [Test]
    public void Every_outcome_has_a_distinct_tag_value_and_a_settled_permit_answer()
    {
        var outcomes = Enum.GetValues<RepoContextGrainStorageWriteGateOutcome>();

        Assert.Multiple(() =>
        {
            Assert.That(outcomes.Select(o => o.TagValue()).Distinct(StringComparer.Ordinal).Count(),
                Is.EqualTo(outcomes.Length),
                "A shared tag value would silently merge two admission arms in the exposition.");
            Assert.That(RepoContextGrainStorageWriteGateOutcome.Immediate.HoldsPermit(), Is.True);
            Assert.That(RepoContextGrainStorageWriteGateOutcome.Queued.HoldsPermit(), Is.True);
            Assert.That(RepoContextGrainStorageWriteGateOutcome.TimedOut.HoldsPermit(), Is.False);
            Assert.That(RepoContextGrainStorageWriteGateOutcome.Unbounded.HoldsPermit(), Is.False);
            Assert.Throws<ArgumentOutOfRangeException>(
                () => ((RepoContextGrainStorageWriteGateOutcome)99).TagValue());
        });
    }

    private static void InterlockedRaise(ref int target, int value)
    {
        var observed = Volatile.Read(ref target);
        while (value > observed)
        {
            var seen = Interlocked.CompareExchange(ref target, value, observed);
            if (seen == observed)
            {
                return;
            }

            observed = seen;
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + Generous;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                Assert.Fail("The gate did not reach the expected state within the timeout.");
            }

            await Task.Delay(5);
        }
    }
}
