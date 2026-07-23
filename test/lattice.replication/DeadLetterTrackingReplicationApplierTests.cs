using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public partial class DeadLetterTrackingReplicationApplierTests
{
    private const string TreeId = "tree";

    private static WalRecord MakeEntry(string key = "k", MutationKind op = MutationKind.Set) => new()
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
        var (decorator, inner, dlq, hwm, options, _) = BuildWithFactory(maxRetries);
        return (decorator, inner, dlq, hwm, options);
    }

    private static (DeadLetterTrackingReplicationApplier decorator,
                    IReplicationApplier inner,
                    IReplicationDeadLetterGrain dlq,
                    IReplicationHighWaterMarkGrain hwm,
                    LatticeReplicationOptions options,
                    IGrainFactory grainFactory) BuildWithFactory(int maxRetries)
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

        var decorator = new DeadLetterTrackingReplicationApplier(
            inner, grainFactory, monitor, NullLogger<DeadLetterTrackingReplicationApplier>.Instance);
        return (decorator, inner, dlq, hwm, options, grainFactory);
    }

    [Test]
    public async Task ApplyAsync_returns_inner_result_on_success()
    {
        var (decorator, inner, _, _, _) = Build(maxRetries: 3);
        var expected = new ApplyResult { Applied = true };
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(expected);

        var result = await decorator.ApplyAsync(MakeEntry(), CancellationToken.None);

        Assert.That(result, Is.EqualTo(expected));
    }

    [Test]
    public void ApplyAsync_rethrows_failure_below_threshold()
    {
        var (decorator, inner, _, _, _) = Build(maxRetries: 3);
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("boom"));

        Assert.That(
            async () => await decorator.ApplyAsync(MakeEntry(), CancellationToken.None),
            Throws.InvalidOperationException);
    }

    [Test]
    public async Task ApplyAsync_parks_entry_when_threshold_reached()
    {
        var (decorator, inner, dlq, hwm, _) = Build(maxRetries: 2);
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
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
        await dlq.Received(1).EnqueueAsync(entry, "boom", 2, LatticeReplicationMetrics.ReasonSchema, Arg.Any<CancellationToken>());
        await hwm.Received(1).TryAdvanceAsync(entry.OriginClusterId!, entry.Timestamp, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_skips_hwm_advance_for_range_deletes()
    {
        var (decorator, inner, dlq, hwm, _) = Build(maxRetries: 1);
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("boom"));

        var entry = MakeEntry(op: MutationKind.DeleteRange);
        var result = await decorator.ApplyAsync(entry, CancellationToken.None);

        Assert.That(result.Applied, Is.False);
        await dlq.Received(1).EnqueueAsync(entry, Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
        await hwm.DidNotReceive().TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_skips_hwm_advance_for_tx_commit_terminal()
    {
        // The canonical ReplicationApplier routes TxCommit / TxAbort
        // through ApplyTxTerminalCoreAsync, which deliberately bypasses
        // the per-origin HWM (saga terminal HLCs are saga linearization
        // points, not per-origin frontiers; the receiver dedupes
        // terminals through the per-tree TxRegistry instead). The
        // DLQ-park path must preserve this invariant: advancing HWM
        // past a parked terminal would silently dedupe any in-flight
        // retry of a legitimate point mutation from the same origin
        // carrying an HLC at or below the terminal's HLC.
        var (decorator, inner, dlq, hwm, _) = Build(maxRetries: 1);
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("shard unavailable"));

        var entry = MakeEntry(op: MutationKind.TxCommit);
        var result = await decorator.ApplyAsync(entry, CancellationToken.None);

        Assert.That(result.Applied, Is.False);
        await dlq.Received(1).EnqueueAsync(entry, Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
        await hwm.DidNotReceive().TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_skips_hwm_advance_for_tx_abort_terminal()
    {
        // Mirror of the TxCommit carveout: TxAbort routes through the
        // same ApplyTxTerminalCoreAsync seam and must not consume a
        // per-origin HWM slot on DLQ park.
        var (decorator, inner, dlq, hwm, _) = Build(maxRetries: 1);
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("shard unavailable"));

        var entry = MakeEntry(op: MutationKind.TxAbort);
        var result = await decorator.ApplyAsync(entry, CancellationToken.None);

        Assert.That(result.Applied, Is.False);
        await dlq.Received(1).EnqueueAsync(entry, Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
        await hwm.DidNotReceive().TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_clears_failure_counter_on_subsequent_success()
    {
        var (decorator, inner, dlq, _, _) = Build(maxRetries: 3);
        var calls = 0;
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
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
        await dlq.DidNotReceive().EnqueueAsync(Arg.Any<WalRecord>(), Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string>(), Arg.Any<CancellationToken>());

        // The next failure now starts a fresh budget; should rethrow, not park.
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("boom-2"));
        Assert.That(async () => await decorator.ApplyAsync(entry, CancellationToken.None), Throws.InvalidOperationException);
        await dlq.DidNotReceive().EnqueueAsync(Arg.Any<WalRecord>(), Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void ApplyAsync_propagates_cancellation_without_counting_as_failure()
    {
        var (decorator, inner, _, _, _) = Build(maxRetries: 1);
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
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

    [Test]
    public async Task ApplyAsync_classifies_argument_exception_as_schema_reason()
    {
        var (decorator, inner, dlq, _, _) = Build(maxRetries: 1);
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new ArgumentException("missing field"));

        var entry = MakeEntry();
        await decorator.ApplyAsync(entry, CancellationToken.None);

        await dlq.Received(1).EnqueueAsync(
            entry,
            "missing field",
            1,
            LatticeReplicationMetrics.ReasonSchema,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_classifies_invalid_operation_exception_as_schema_reason()
    {
        var (decorator, inner, dlq, _, _) = Build(maxRetries: 1);
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("bad mode"));

        var entry = MakeEntry();
        await decorator.ApplyAsync(entry, CancellationToken.None);

        await dlq.Received(1).EnqueueAsync(
            entry,
            "bad mode",
            1,
            LatticeReplicationMetrics.ReasonSchema,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_classifies_other_exceptions_as_unknown_reason()
    {
        var (decorator, inner, dlq, _, _) = Build(maxRetries: 1);
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new TimeoutException("transport timed out"));

        var entry = MakeEntry();
        await decorator.ApplyAsync(entry, CancellationToken.None);

        await dlq.Received(1).EnqueueAsync(
            entry,
            "transport timed out",
            1,
            LatticeReplicationMetrics.ReasonUnknown,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_contains_empty_TreeId_entry_without_wedging()
    {
        // Fail-safe backstop for the #1331 convergence wedge. A
        // structurally-invalid entry with an empty TreeId cannot be applied
        // to any tree (the canonical applier rejects it up front) and cannot
        // be quarantined either - the per-tree dead-letter grain and the
        // per-origin high-water-mark grain are both keyed on the tree id, so
        // GetGrain throws ArgumentException on the empty key before the entry
        // is parked or the cursor advanced. Left unguarded, a single such
        // entry becomes a permanent convergence wedge. The decorator must
        // contain it: return a non-applied result without throwing and
        // without keying any grain on the empty id.
        var (decorator, inner, _, _, _, grainFactory) = BuildWithFactory(maxRetries: 1);
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new ArgumentException("WalRecord.TreeId must be non-empty."));

        // Match production: Orleans rejects an empty grain primary key.
        grainFactory.GetGrain<IReplicationDeadLetterGrain>(string.Empty)
            .Returns(_ => throw new ArgumentException("primaryKey"));
        grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(string.Empty)
            .Returns(_ => throw new ArgumentException("primaryKey"));

        using var enqueued = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, "orleans.lattice.replication.dead_letter.enqueued");

        var entry = MakeEntry() with { TreeId = string.Empty };

        ApplyResult result = default!;
        Assert.That(
            async () => result = await decorator.ApplyAsync(entry, CancellationToken.None),
            Throws.Nothing);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });

        // The empty-key quarantine grains were never touched.
        grainFactory.DidNotReceive().GetGrain<IReplicationDeadLetterGrain>(string.Empty);
        grainFactory.DidNotReceive().GetGrain<IReplicationHighWaterMarkGrain>(string.Empty);

        // The drop is still surfaced on the dead-letter metric, tagged schema.
        var measurement = enqueued.Measurements.Single();
        Assert.That(measurement.Value, Is.EqualTo(1));
        Assert.That(
            measurement.Tags,
            Has.Some.Matches<KeyValuePair<string, object?>>(
                t => t.Key == LatticeReplicationMetrics.TagReason
                    && (string?)t.Value == LatticeReplicationMetrics.ReasonSchema));
    }

    [Test]
    public async Task ApplyAsync_contains_whitespace_TreeId_entry_without_wedging()
    {
        var (decorator, inner, _, _, _, grainFactory) = BuildWithFactory(maxRetries: 1);
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new ArgumentException("bad tree"));

        var entry = MakeEntry() with { TreeId = "   " };

        ApplyResult result = default!;
        Assert.That(
            async () => result = await decorator.ApplyAsync(entry, CancellationToken.None),
            Throws.Nothing);

        Assert.That(result.Applied, Is.False);
        grainFactory.DidNotReceive().GetGrain<IReplicationDeadLetterGrain>("   ");
    }
}
