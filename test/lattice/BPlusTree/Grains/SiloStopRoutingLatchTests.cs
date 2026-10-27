using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="SiloStopRoutingLatch"/> (issue #5100).
/// </summary>
[TestFixture]
public sealed class SiloStopRoutingLatchTests
{
    private const string Withdrawn =
        "No active nodes are compatible with grain walmaterialiserpin and interface ol.wpi version 0. "
        + "Known nodes with grain type: none. All known nodes compatible with interface version: none";

    private const string VersionMismatch =
        "No active nodes are compatible with grain walmaterialiserpin and interface ol.wpi version 3. "
        + "Known nodes with grain type: S10.0.0.1:11111:1. All known nodes compatible with interface version: none";

    [Test]
    public void Does_not_latch_while_the_host_is_running()
    {
        var latch = new SiloStopRoutingLatch(() => false);

        Assert.Multiple(() =>
        {
            Assert.That(latch.Observe(StopRoutedGrainType.MaterialiserPin, new InvalidOperationException(Withdrawn)), Is.False);
            Assert.That(latch.IsWithdrawn(StopRoutedGrainType.MaterialiserPin), Is.False);
        });
    }

    [Test]
    public void Latches_once_on_the_withdrawn_shape_while_stopping()
    {
        var latch = new SiloStopRoutingLatch(() => true);

        Assert.Multiple(() =>
        {
            Assert.That(latch.Observe(StopRoutedGrainType.MaterialiserPin, new InvalidOperationException(Withdrawn)), Is.True);
            Assert.That(latch.Observe(StopRoutedGrainType.MaterialiserPin, new InvalidOperationException(Withdrawn)), Is.False,
                "a second observation of an already-latched type must not report a new latch");
            Assert.That(latch.IsWithdrawn(StopRoutedGrainType.MaterialiserPin), Is.True);
        });
    }

    [Test]
    public void Local_refusal_and_version_mismatch_do_not_latch()
    {
        var latch = new SiloStopRoutingLatch(() => true);

        Assert.Multiple(() =>
        {
            Assert.That(latch.Observe(StopRoutedGrainType.MaterialiserPin, new InvalidOperationException(
                "Unable to create local activation for grain wal-materialiser-pin. Rejecting now.")), Is.False,
                "the local-refusal shape occurs on rolling stops while other silos still host the type");
            Assert.That(latch.Observe(StopRoutedGrainType.MaterialiserPin, new InvalidOperationException(VersionMismatch)), Is.False,
                "a version mismatch names hosting silos and occurs on rolling upgrades");
            Assert.That(latch.Observe(StopRoutedGrainType.MaterialiserPin, new TimeoutException("timed out")), Is.False);
            Assert.That(latch.IsWithdrawn(StopRoutedGrainType.MaterialiserPin), Is.False);
        });
    }

    [Test]
    public void Matches_the_withdrawn_shape_through_inner_and_aggregate_exceptions()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SiloStopRoutingLatch.IsCatalogWithdrawn(
                new InvalidOperationException("outer", new InvalidOperationException(Withdrawn))), Is.True);
            Assert.That(SiloStopRoutingLatch.IsCatalogWithdrawn(
                new AggregateException(new TimeoutException("x"), new InvalidOperationException(Withdrawn))), Is.True);
            Assert.That(SiloStopRoutingLatch.IsCatalogWithdrawn(
                new AggregateException(new TimeoutException("x"))), Is.False);
        });
    }

    [Test]
    public void A_latched_type_is_not_withdrawn_when_the_stop_signal_is_clear()
    {
        var stopping = true;
        var latch = new SiloStopRoutingLatch(() => stopping);
        latch.Observe(StopRoutedGrainType.ReplayCoordinator, new InvalidOperationException(Withdrawn));

        stopping = false;

        Assert.That(latch.IsWithdrawn(StopRoutedGrainType.ReplayCoordinator), Is.False,
            "the latch must only ever suppress calls on a stopping host");
    }

    [Test]
    public void Grain_types_latch_independently()
    {
        var latch = new SiloStopRoutingLatch(() => true);
        latch.Observe(StopRoutedGrainType.ReplayCoordinator, new InvalidOperationException(Withdrawn));

        Assert.Multiple(() =>
        {
            Assert.That(latch.IsWithdrawn(StopRoutedGrainType.ReplayCoordinator), Is.True);
            Assert.That(latch.IsWithdrawn(StopRoutedGrainType.MaterialiserPin), Is.False);
        });
    }

    [Test]
    public void Null_fault_and_none_type_are_ignored()
    {
        var latch = new SiloStopRoutingLatch(() => true);

        Assert.Multiple(() =>
        {
            Assert.That(latch.Observe(StopRoutedGrainType.MaterialiserPin, null), Is.False);
            Assert.That(latch.Observe(StopRoutedGrainType.None, new InvalidOperationException(Withdrawn)), Is.False);
            Assert.That(latch.IsWithdrawn(StopRoutedGrainType.None), Is.False);
        });
    }

    [Test]
    public void Di_constructor_without_a_host_lifetime_never_reports_stopping()
    {
        var latch = new SiloStopRoutingLatch(new ServiceCollection().BuildServiceProvider());

        Assert.Multiple(() =>
        {
            Assert.That(latch.IsStopping, Is.False);
            Assert.That(latch.Observe(StopRoutedGrainType.MaterialiserPin, new InvalidOperationException(Withdrawn)), Is.False);
        });
    }

    [Test]
    public void Di_constructor_tracks_the_host_application_stopping_token()
    {
        using var stopping = new CancellationTokenSource();
        var lifetime = Substitute.For<IHostApplicationLifetime>();
        lifetime.ApplicationStopping.Returns(stopping.Token);
        var services = new ServiceCollection().AddSingleton(lifetime).BuildServiceProvider();
        var latch = new SiloStopRoutingLatch(services);

        Assert.That(latch.IsStopping, Is.False);

        stopping.Cancel();

        Assert.Multiple(() =>
        {
            Assert.That(latch.IsStopping, Is.True);
            Assert.That(latch.Observe(StopRoutedGrainType.MaterialiserPin, new InvalidOperationException(Withdrawn)), Is.True);
            Assert.That(latch.IsWithdrawn(StopRoutedGrainType.MaterialiserPin), Is.True);
        });
    }
}
