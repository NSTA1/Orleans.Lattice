using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression tests for inbound-contact attribution through the
/// <c>DeadLetterTrackingReplicationApplier</c>, which is what
/// <c>IReplicationApplier</c> resolves to on a real silo. The canonical applier
/// records the inbound contact on its batch entry point only, so a branch of the
/// decorator's <c>ApplyBatchAsync</c> that applies entries one at a time - the
/// single-entry push of a low-rate sender, and the per-entry slow path - used to
/// record nothing, and a receiver never reported an inbound link for such a
/// peer. Found by the #3812 two-region peer-status integration test.
/// </summary>
public partial class DeadLetterTrackingReplicationApplierTests
{
    private const string RemoteOrigin = "site-b";

    private static (DeadLetterTrackingReplicationApplier Decorator, IReplicationApplier Inner, ReplicationPeerStats Stats)
        BuildWithStats(int maxRetries = 3)
    {
        var inner = Substitute.For<IReplicationApplier>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IReplicationDeadLetterGrain>(TreeId).Returns(Substitute.For<IReplicationDeadLetterGrain>());
        grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>())
            .Returns(Substitute.For<IReplicationHighWaterMarkGrain>());
        var options = new LatticeReplicationOptions { ClusterId = "site-a", MaxApplyRetries = maxRetries };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        var stats = new ReplicationPeerStats();

        var decorator = new DeadLetterTrackingReplicationApplier(
            inner, grainFactory, monitor, NullLogger<DeadLetterTrackingReplicationApplier>.Instance, stats);
        return (decorator, inner, stats);
    }

    private static ReplicationPeerSnapshot? Inbound(ReplicationPeerStats stats) =>
        stats.Snapshot()
            .Where(s => s.Direction == ReplicationContactDirection.Inbound && s.Tree == TreeId && s.Peer == RemoteOrigin)
            .Cast<ReplicationPeerSnapshot?>()
            .SingleOrDefault();

    private static ApplyResult Applied(WalRecord entry) => new() { Applied = true, HighWaterMark = entry.Timestamp };

    [Test]
    public async Task ApplyBatchAsync_single_entry_records_the_inbound_contact()
    {
        var (decorator, inner, stats) = BuildWithStats();
        var entry = MakeEntry("a");
        inner.ApplyAsync(entry, Arg.Any<CancellationToken>()).Returns(Applied(entry));

        await decorator.ApplyBatchAsync(new[] { entry }, CancellationToken.None);

        var row = Inbound(stats);
        Assert.Multiple(() =>
        {
            Assert.That(row, Is.Not.Null, "a one-entry push must be attributed to its origin peer");
            Assert.That(double.IsNaN(row!.Value.LastContactSeconds), Is.False);
            Assert.That(row.Value.ConsecutiveErrors, Is.Zero);
        });
    }

    [Test]
    public async Task ApplyBatchAsync_single_entry_failure_records_an_inbound_error()
    {
        var (decorator, inner, stats) = BuildWithStats(maxRetries: 3);
        var entry = MakeEntry("a");
        inner.ApplyAsync(entry, Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("boom"));

        Assert.That(
            async () => await decorator.ApplyBatchAsync(new[] { entry }, CancellationToken.None),
            Throws.InvalidOperationException);

        var row = Inbound(stats);
        Assert.Multiple(() =>
        {
            Assert.That(row?.ConsecutiveErrors, Is.EqualTo(1));
            Assert.That(double.IsNaN(row!.Value.LastContactSeconds), Is.True);
        });
    }

    [Test]
    public async Task ApplyBatchAsync_slow_path_records_the_inbound_contact()
    {
        var (decorator, inner, stats) = BuildWithStats();
        inner.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("batch boom"));
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(Applied(call.Arg<WalRecord>())));

        await decorator.ApplyBatchAsync(new[] { MakeEntry("a"), MakeEntry("b") }, CancellationToken.None);

        var row = Inbound(stats);
        Assert.Multiple(() =>
        {
            Assert.That(row, Is.Not.Null);
            Assert.That(double.IsNaN(row!.Value.LastContactSeconds), Is.False);
            Assert.That(row.Value.ConsecutiveErrors, Is.Zero, "the successful per-entry applies reset the streak");
        });
    }

    [Test]
    public async Task ApplyBatchAsync_fast_path_leaves_recording_to_the_inner_batch_path()
    {
        var (decorator, inner, stats) = BuildWithStats();
        inner.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(new ApplyResult { Applied = true });

        await decorator.ApplyBatchAsync(new[] { MakeEntry("a"), MakeEntry("b") }, CancellationToken.None);

        Assert.That(Inbound(stats), Is.Null, "the inner batch path records the contact; recording here would double it");
    }

    [Test]
    public async Task ApplyAsync_called_directly_does_not_record_the_inbound_contact()
    {
        var (decorator, inner, stats) = BuildWithStats();
        var entry = MakeEntry("a");
        inner.ApplyAsync(entry, Arg.Any<CancellationToken>()).Returns(Applied(entry));

        await decorator.ApplyAsync(entry, CancellationToken.None);

        Assert.That(Inbound(stats), Is.Null, "the per-entry entry point matches the canonical applier, which records only on its batch path");
    }

    [Test]
    public async Task ApplyBatchAsync_does_not_attribute_a_local_origin_entry()
    {
        var (decorator, inner, stats) = BuildWithStats();
        var local = MakeEntry("a") with { OriginClusterId = "site-a" };
        inner.ApplyAsync(local, Arg.Any<CancellationToken>()).Returns(Applied(local));

        await decorator.ApplyBatchAsync(new[] { local }, CancellationToken.None);

        Assert.That(stats.Snapshot(), Is.Empty);
    }

    [Test]
    public void ApplyBatchAsync_cancellation_records_nothing()
    {
        var (decorator, inner, stats) = BuildWithStats();
        var entry = MakeEntry("a");
        inner.ApplyAsync(entry, Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new OperationCanceledException());

        Assert.That(
            async () => await decorator.ApplyBatchAsync(new[] { entry }, CancellationToken.None),
            Throws.InstanceOf<OperationCanceledException>());
        Assert.That(stats.Snapshot(), Is.Empty);
    }

    [Test]
    public async Task ApplyBatchAsync_without_telemetry_state_is_unchanged()
    {
        var (decorator, inner, _, _, _) = Build(maxRetries: 3);
        var entry = MakeEntry("a");
        inner.ApplyAsync(entry, Arg.Any<CancellationToken>()).Returns(Applied(entry));

        var result = await decorator.ApplyBatchAsync(new[] { entry }, CancellationToken.None);

        Assert.That(result.Applied, Is.True);
    }
}
