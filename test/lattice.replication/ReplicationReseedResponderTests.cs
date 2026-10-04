using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4534: the receiver's answer to a sender that took it off the log.
/// The coordinator grain is the only stand-in.
/// </summary>
[TestFixture]
public class ReplicationReseedResponderTests
{
    private const string Tree = "reseed-tree";
    private const string Source = "site-a";

    private static (IGrainFactory Factory, ILatticeBootstrapCoordinatorGrain Coordinator) Create(
        long? completed, string? runningSource = null)
    {
        var coordinator = Substitute.For<ILatticeBootstrapCoordinatorGrain>();
        coordinator.GetCompletedExportEpochAsync(Source).Returns(Task.FromResult(completed));
        coordinator.GetStatusAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new BootstrapCoordinatorStatus(LatticeBootstrapState.Idle, runningSource)));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeBootstrapCoordinatorGrain>(Tree).Returns(coordinator);
        return (factory, coordinator);
    }

    [Test]
    public async Task Echoes_a_completed_reseed_past_the_requested_epoch_without_bootstrapping_again()
    {
        var (factory, coordinator) = Create(completed: 5);

        var echoed = await ReplicationReseedResponder.RespondAsync(factory, Tree, Source, 3, true, NullLogger.Instance);

        Assert.That(echoed, Is.EqualTo(5));
        await coordinator.DidNotReceiveWithAnyArgs().BootstrapAsync(default!, default);
    }

    [Test]
    public async Task Starts_a_bootstrap_when_none_has_completed_past_the_requested_epoch()
    {
        var (factory, coordinator) = Create(completed: 3);

        var echoed = await ReplicationReseedResponder.RespondAsync(factory, Tree, Source, 3, true, NullLogger.Instance);

        Assert.That(echoed, Is.EqualTo(3));
        await coordinator.Received(1).BootstrapAsync(Source, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Does_not_start_a_second_bootstrap_while_one_is_running()
    {
        var (factory, coordinator) = Create(completed: null, runningSource: Source);

        await ReplicationReseedResponder.RespondAsync(factory, Tree, Source, 0, true, NullLogger.Instance);

        await coordinator.DidNotReceiveWithAnyArgs().BootstrapAsync(default!, default);
    }

    [Test]
    public async Task Leaves_the_bootstrap_to_the_operator_when_auto_bootstrap_is_disabled()
    {
        var (factory, coordinator) = Create(completed: null);

        var echoed = await ReplicationReseedResponder.RespondAsync(factory, Tree, Source, 0, false, NullLogger.Instance);

        Assert.That(echoed, Is.Null);
        await coordinator.DidNotReceiveWithAnyArgs().BootstrapAsync(default!, default);
    }

    [Test]
    public async Task A_failed_lookup_echoes_nothing_so_the_sender_keeps_withholding()
    {
        var (factory, coordinator) = Create(completed: 9);
        coordinator.GetCompletedExportEpochAsync(Source).ThrowsAsync(new TimeoutException("simulated"));

        var echoed = await ReplicationReseedResponder.RespondAsync(factory, Tree, Source, 0, true, NullLogger.Instance);

        Assert.That(echoed, Is.Null);
    }
}
