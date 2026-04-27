using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class DeadLetterTrackingReplicationApplierTests
{
    private const string TreeId = "tree";

    private static ReplogEntry MakeEntry(string key = "k", ReplogOp op = ReplogOp.Set) => new()
    {
        TreeId = TreeId,
        Op = op,
        Key = key,
        Value = new byte[] { 1, 2, 3 },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = "site-b",
    };

    private static (DeadLetterTrackingReplicationApplier decorator,
                    IReplicationApplier inner,
                    IReplicationDeadLetterGrain dlq,
                    IReplicationHighWaterMarkGrain hwm,
                    LatticeReplicationOptions options) Build(int maxRetries)
    {
        var inner = Substitute.For<IReplicationApplier>();
        var dlq = Substitute.For<IReplicationDeadLetterGrain>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IReplicationDeadLetterGrain>(TreeId).Returns(dlq);
        grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(hwm);

        var options = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            MaxApplyRetries = maxRetries,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);

        var decorator = new DeadLetterTrackingReplicationApplier(inner, grainFactory, monitor);
        return (decorator, inner, dlq, hwm, options);
    }

    [Test]
    public async Task ApplyAsync_returns_inner_result_on_success()
    {
        var (decorator, inner, _, _, _) = Build(maxRetries: 3);
        var expected = new ApplyResult { Applied = true };
        inner.ApplyAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>()).Returns(expected);

        var result = await decorator.ApplyAsync(MakeEntry(), CancellationToken.None);

        Assert.That(result, Is.EqualTo(expected));
    }

    [Test]
    public void ApplyAsync_rethrows_failure_below_threshold()
    {
        var (decorator, inner, _, _, _) = Build(maxRetries: 3);
        inner.ApplyAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("boom"));

        Assert.That(
            async () => await decorator.ApplyAsync(MakeEntry(), CancellationToken.None),
            Throws.InvalidOperationException);
    }

    [Test]
    public async Task ApplyAsync_parks_entry_when_threshold_reached()
    {
        var (decorator, inner, dlq, hwm, _) = Build(maxRetries: 2);
        inner.ApplyAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("boom"));

        var entry = MakeEntry();
        Assert.That(async () => await decorator.ApplyAsync(entry, CancellationToken.None),
            Throws.InvalidOperationException);
        var result = await decorator.ApplyAsync(entry, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });
        await dlq.Received(1).EnqueueAsync(entry, "boom", 2, Arg.Any<CancellationToken>());
        await hwm.Received(1).TryAdvanceAsync(entry.Timestamp, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_skips_hwm_advance_for_range_deletes()
    {
        var (decorator, inner, dlq, hwm, _) = Build(maxRetries: 1);
        inner.ApplyAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("boom"));

        var entry = MakeEntry(op: ReplogOp.DeleteRange);
        var result = await decorator.ApplyAsync(entry, CancellationToken.None);

        Assert.That(result.Applied, Is.False);
        await dlq.Received(1).EnqueueAsync(entry, Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
        await hwm.DidNotReceive().TryAdvanceAsync(Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_clears_failure_counter_on_subsequent_success()
    {
        var (decorator, inner, dlq, _, _) = Build(maxRetries: 3);
        var calls = 0;
        inner.ApplyAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ =>
            {
                calls++;
                if (calls <= 2)
                {
                    throw new InvalidOperationException("boom");
                }
                return Task.FromResult(new ApplyResult { Applied = true });
            });

        var entry = MakeEntry();
        Assert.That(async () => await decorator.ApplyAsync(entry, CancellationToken.None), Throws.InvalidOperationException);
        Assert.That(async () => await decorator.ApplyAsync(entry, CancellationToken.None), Throws.InvalidOperationException);
        var success = await decorator.ApplyAsync(entry, CancellationToken.None);

        Assert.That(success.Applied, Is.True);
        await dlq.DidNotReceive().EnqueueAsync(Arg.Any<ReplogEntry>(), Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>());

        // The next failure now starts a fresh budget; should rethrow, not park.
        inner.ApplyAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("boom-2"));
        Assert.That(async () => await decorator.ApplyAsync(entry, CancellationToken.None), Throws.InvalidOperationException);
        await dlq.DidNotReceive().EnqueueAsync(Arg.Any<ReplogEntry>(), Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void ApplyAsync_propagates_cancellation_without_counting_as_failure()
    {
        var (decorator, inner, _, _, _) = Build(maxRetries: 1);
        inner.ApplyAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new OperationCanceledException());

        Assert.That(
            async () => await decorator.ApplyAsync(MakeEntry(), CancellationToken.None),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void ApplyAsync_observes_pre_cancelled_token()
    {
        var (decorator, _, _, _, _) = Build(maxRetries: 1);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await decorator.ApplyAsync(MakeEntry(), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
