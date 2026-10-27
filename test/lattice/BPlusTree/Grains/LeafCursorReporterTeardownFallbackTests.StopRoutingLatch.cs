using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5100: during a whole-cluster stop every flush re-sent a pin-grain call
/// that could only fail with the catalog-withdrawn placement error, and Orleans
/// logged each one at Error before the #3382 fallback caught it. Once a stopping
/// silo has seen that shape, the reporter must write pins straight to the durable
/// slot without calling the grain, and must still land the monotonic maximum.
/// </summary>
public sealed partial class LeafCursorReporterTeardownFallbackTests
{
    [Test]
    public async Task Stopping_silo_skips_pin_grain_after_catalog_withdrawn_fault()
    {
        var pin = new CountingWithdrawnPinGrain();
        var latch = new SiloStopRoutingLatch(() => true);
        var (reporter, storage) = Create(pin, latch);

        await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, Reports((ConsumerA, Hlc(100))), CancellationToken.None);
        Assert.That(pin.Calls, Is.EqualTo(1), "the first flush must try the grain and observe the fault");

        var acknowledged = await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, Reports((ConsumerA, Hlc(300))), CancellationToken.None);
        await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, Reports((ConsumerA, Hlc(200))), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pin.Calls, Is.EqualTo(1),
                "once latched, no further pin-grain call may be sent from the stopping silo");
            Assert.That(acknowledged, Is.True, "the direct-store write must acknowledge the flush");
            Assert.That(storage.TryReadPin(Tree, ConsumerA, out var pinned), Is.True);
            Assert.That(pinned, Is.EqualTo(Hlc(300)),
                "the bypass must keep the monotonic-max merge: a lower frontier never regresses the pin");
        });
    }

    [Test]
    public async Task Running_silo_keeps_calling_the_pin_grain_after_a_withdrawn_fault()
    {
        var pin = new CountingWithdrawnPinGrain();
        var latch = new SiloStopRoutingLatch(() => false);
        var (reporter, storage) = Create(pin, latch);

        await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, Reports((ConsumerA, Hlc(100))), CancellationToken.None);
        await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, Reports((ConsumerA, Hlc(200))), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pin.Calls, Is.EqualTo(2),
                "a host that is not stopping must never latch, so every flush tries the grain");
            Assert.That(storage.TryReadPin(Tree, ConsumerA, out var pinned), Is.True);
            Assert.That(pinned, Is.EqualTo(Hlc(200)));
        });
    }

    [Test]
    public async Task Local_activation_refusal_does_not_latch_while_stopping()
    {
        // The #1464 refusal shape is raised by a single stopping silo during a
        // rolling stop, while other silos still host the pin grain, so it must not
        // stop this silo routing to the grain.
        var pin = new CountingRejectingPinGrain();
        var latch = new SiloStopRoutingLatch(() => true);
        var (reporter, _) = Create(pin, latch);

        await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, Reports((ConsumerA, Hlc(100))), CancellationToken.None);
        await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, Reports((ConsumerA, Hlc(200))), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pin.Calls, Is.EqualTo(2));
            Assert.That(latch.IsWithdrawn(StopRoutedGrainType.MaterialiserPin), Is.False);
        });
    }

    private sealed class CountingWithdrawnPinGrain : PinGrainStub
    {
        private int _calls;

        public int Calls => Volatile.Read(ref _calls);

        public override Task ReportManyAsync(IReadOnlyList<MaterialiserPinReport> reports) => Withdraw();

        public override Task SeedManyAsync(IReadOnlyList<MaterialiserPinReport> reports) => Withdraw();

        private Task Withdraw()
        {
            Interlocked.Increment(ref _calls);
            return Task.FromException(new InvalidOperationException(
                "No active nodes are compatible with grain walmaterialiserpin and interface " +
                "ol.wpi version 0. Known nodes with grain type: none. All known nodes compatible " +
                "with interface version: none"));
        }
    }

    private sealed class CountingRejectingPinGrain : PinGrainStub
    {
        private int _calls;

        public int Calls => Volatile.Read(ref _calls);

        public override Task ReportManyAsync(IReadOnlyList<MaterialiserPinReport> reports) => Reject();

        public override Task SeedManyAsync(IReadOnlyList<MaterialiserPinReport> reports) => Reject();

        private Task Reject()
        {
            Interlocked.Increment(ref _calls);
            return Task.FromException(new InvalidOperationException(
                "Unable to create local activation for grain wal-materialiser-pin. Rejecting now."));
        }
    }
}
