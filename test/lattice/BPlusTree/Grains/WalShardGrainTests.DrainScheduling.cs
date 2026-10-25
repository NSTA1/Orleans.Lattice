using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class WalShardGrainTests
{
    [Test]
    public async Task OnDeactivateAsync_waits_for_a_live_slot_to_settle_without_a_budget_fault()
    {
        using var capture = new DrainBudgetMeterCapture();
        var provider = new UncancellableHangingProvider();
        var grain = await CreateGrainAsync(provider, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 1,
            WalDrainBudget = TimeSpan.FromSeconds(10),
            WalFlushTimeout = TimeSpan.FromSeconds(60),
            WalFlushPreflightTimeout = Timeout.InfiniteTimeSpan,
        });
        var append = grain.AppendAsync(MakeEntry("live"), CancellationToken.None);
        await WaitForAppendInvocationsAsync(() => Volatile.Read(ref provider.AppendInvocations), 1,
            "the natural drain needs a real in-flight provider operation");

        var drain = grain.OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "test"), CancellationToken.None);
        try
        {
            await WaitForAppendInvocationsAsync(() => provider.HighestInvocations, 2,
                "the cancelled flush must be parked in its real failure resync before observing drain completion");
            Assert.That(drain.IsCompleted, Is.False,
                "deactivation must await the live slot's terminal resync, not just observe cancellation");
        }
        finally
        {
            provider.Released.TrySetResult();
            await drain.WaitAsync(TimeSpan.FromSeconds(5));
        }

        Assert.That(async () => await append, Throws.InstanceOf<TimeoutException>());
        Assert.Multiple(() =>
        {
            Assert.That(capture.Count("orleans.lattice.wal.shard.drain.budget.expirations"), Is.Zero);
            Assert.That(capture.FirstFor("orleans.lattice.wal.shard.drain.budget.force_faulted_slots"), Is.Null,
                "a live slot settled before its budget, so no forced expiration is attributable");
        });
    }
}
