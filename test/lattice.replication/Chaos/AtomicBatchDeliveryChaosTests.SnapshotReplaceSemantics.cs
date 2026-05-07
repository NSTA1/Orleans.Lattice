using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Snapshot replace-semantics test (G7) for the cross-cluster
/// atomic-batch surface. Pins the contract that
/// <see cref="ISnapshotProvider.ExportAsync"/> recomputes
/// <see cref="SnapshotStream.SagaBlacklist"/> from the current
/// in-flight saga set on every call - the blacklist is a
/// per-export view of the producer's current state, not an
/// accumulated history. A regression that accumulated blacklisted
/// ids across exports would surface as ever-growing snapshot
/// streams that bloat receiver-side blacklist sets and starve
/// causal-stream progress for already-completed sagas.
/// <para>
/// Three sequential exports drive the contract: (1) saga A is
/// half-emitted -&gt; blacklist contains A; (2) saga A completes
/// (the tracker drains its row) -&gt; blacklist is empty (replace,
/// not accumulate); (3) saga B is half-emitted -&gt; blacklist
/// contains B but not A.
/// </para>
/// </summary>
public partial class AtomicBatchDeliveryChaosTests
{
    [Test]
    public async Task Snapshot_blacklist_recomputed_per_export_and_drops_completed_sagas()
    {
        await using var harness = new SnapshotDuringSagaHarness();
        await harness.InitializeAsync();
        var sagaTracker = harness.SagaTracker;
        var snapshotProvider = harness.SnapshotProvider;

        // Step 1: saga A half-emits (5 of 10 siblings observed).
        var txA = Guid.NewGuid();
        const int batchSize = 10;
        for (var i = 0; i < batchSize / 2; i++)
        {
            sagaTracker.ObserveEmission(SnapshotTreeId, txA, batchSize);
        }

        // Snapshot #1: A is in flight, must be blacklisted.
        var snap1 = await snapshotProvider.ExportAsync(
            SnapshotTreeId,
            HybridLogicalClock.Zero,
            CancellationToken.None);

        Assert.That(
            snap1.SagaBlacklist,
            Has.Member(txA),
            "Snapshot #1: half-emitted saga A must appear on the blacklist.");
        Assert.That(
            snap1.SagaBlacklist,
            Has.Count.EqualTo(1),
            "Snapshot #1: only saga A is in flight; blacklist must contain exactly one id.");

        // Step 2: saga A completes (remaining 5 emissions drain the
        // tracker row when count reaches batchSize).
        for (var i = batchSize / 2; i < batchSize; i++)
        {
            sagaTracker.ObserveEmission(SnapshotTreeId, txA, batchSize);
        }
        Assert.That(
            sagaTracker.GetInFlightTransactions(SnapshotTreeId),
            Has.No.Member(txA),
            "Pre-condition: saga A must no longer be in flight after every sibling has emitted.");

        // Snapshot #2: A is no longer in flight, blacklist must be
        // empty. This is the replace-semantics pin: a regression
        // that accumulated blacklisted ids across exports would
        // still report A here.
        var snap2 = await snapshotProvider.ExportAsync(
            SnapshotTreeId,
            HybridLogicalClock.Zero,
            CancellationToken.None);

        Assert.That(
            snap2.SagaBlacklist,
            Is.Empty,
            "Snapshot #2: saga A has completed; blacklist must be empty (replace, not accumulate).");

        // Step 3: saga B half-emits (3 of 6 siblings).
        var txB = Guid.NewGuid();
        const int batchSizeB = 6;
        for (var i = 0; i < batchSizeB / 2; i++)
        {
            sagaTracker.ObserveEmission(SnapshotTreeId, txB, batchSizeB);
        }

        var snap3 = await snapshotProvider.ExportAsync(
            SnapshotTreeId,
            HybridLogicalClock.Zero,
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                snap3.SagaBlacklist,
                Has.Member(txB),
                "Snapshot #3: half-emitted saga B must appear on the blacklist.");
            Assert.That(
                snap3.SagaBlacklist,
                Has.No.Member(txA),
                "Snapshot #3: completed saga A must NOT appear on the blacklist.");
            Assert.That(
                snap3.SagaBlacklist,
                Has.Count.EqualTo(1),
                "Snapshot #3: only saga B is in flight; blacklist must contain exactly one id.");
        });
    }
}
